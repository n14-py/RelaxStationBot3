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

# ======================
# CONFIGURACIÓN COMPLETA
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('radio.log')
    ]
)

CONFIG = {
    "MEDIOS_URL": "https://raw.githubusercontent.com/n14-py/RelaxStationmedios/master/mediosmusic.json",
    "CACHE_DIR": os.path.abspath("./radio_cache"),
    "STREAM_DURATION": 8 * 3600,
    "RETRY_DELAY": 300,
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
        "gop": "60",
        "bufsize": "6000k",
        "thumbnail": {
            "quality": 85,
            "max_size": 2 * 1024 * 1024
        }
    },
    "STREAM_STATES": {
        "activation_timeout": 180,
        "check_interval": 15
    }
}

# ===================
# CLASE GESTOR CONTENIDO (COMPLETA)
# ===================
class GestorContenido:
    def __init__(self):
        os.makedirs(CONFIG['CACHE_DIR'], exist_ok=True)
        self.medios = self._cargar_medios()
        self._validar_recursos()

    def _validar_recursos(self):
        if not self.medios['imagenes']:
            logging.critical("No se encontraron imágenes válidas")
        if not self.medios['musica']:
            logging.critical("No se encontraron archivos de audio")

    def _procesar_url_google_drive(self, url):
        try:
            if "drive.google.com" in url:
                if '/d/' in url:
                    file_id = url.split('/d/')[1].split('/')[0]
                else:
                    file_id = url.split('id=')[1].split('&')[0]
                return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            return url
        except Exception as e:
            logging.error(f"Error procesando URL: {str(e)}")
            return url

    def _optimizar_imagen(self, ruta_imagen):
        try:
            logging.info(f"Iniciando optimización de imagen: {os.path.basename(ruta_imagen)}")
            ruta_optimizada = f"{ruta_imagen}_optimizada.jpg"
            
            # Primer paso: Redimensionar
            subprocess.run([
                "ffmpeg", "-y", "-i", ruta_imagen,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2", ruta_optimizada
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # Segundo paso: Comprimir
            calidad = CONFIG['FFMPEG_PARAMS']['thumbnail']['quality']
            while os.path.getsize(ruta_optimizada) > CONFIG['FFMPEG_PARAMS']['thumbnail']['max_size'] and calidad > 30:
                subprocess.run([
                    "ffmpeg", "-y", "-i", ruta_optimizada,
                    "-q:v", str(calidad), ruta_optimizada
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                calidad -= 5
                logging.info(f"Reintentando con calidad: {calidad}%")
            
            return ruta_optimizada if os.path.exists(ruta_optimizada) else ruta_imagen
        except Exception as e:
            logging.error(f"Error en optimización: {str(e)}")
            return ruta_imagen

    def _descargar_recurso(self, url, es_imagen=False):
        try:
            url = self._procesar_url_google_drive(url)
            hash_nombre = hashlib.md5(url.encode()).hexdigest()
            extension = ".jpg" if es_imagen else ".mp3"
            ruta_local = os.path.join(CONFIG['CACHE_DIR'], f"{hash_nombre}{extension}")
            
            if os.path.exists(ruta_local):
                logging.debug(f"Recurso en caché: {ruta_local}")
                return ruta_local
                
            logging.info(f"Descargando {'imagen' if es_imagen else 'audio'}: {url}")
            with requests.get(url, stream=True, timeout=30) as respuesta:
                respuesta.raise_for_status()
                with open(ruta_local, 'wb') as archivo:
                    for chunk in respuesta.iter_content(chunk_size=8192):
                        if chunk:
                            archivo.write(chunk)
            
            return self._optimizar_imagen(ruta_local) if es_imagen else ruta_local
        except Exception as e:
            logging.error(f"Error descarga: {str(e)}")
            return None

    def _cargar_medios(self):
        try:
            logging.info("Iniciando carga de medios...")
            respuesta = requests.get(CONFIG["MEDIOS_URL"], timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            return {
                "imagenes": [self._procesar_imagen(img) for img in datos.get('imagenes', [])],
                "musica": [self._procesar_audio(audio) for audio in datos.get('musica', [])]
            }
        except Exception as e:
            logging.critical(f"Error carga medios: {str(e)}")
            return {"imagenes": [], "musica": []}

    def _procesar_imagen(self, imagen):
        ruta = self._descargar_recurso(imagen['url'], es_imagen=True)
        return {"name": imagen['name'], "local_path": ruta} if ruta else None

    def _procesar_audio(self, audio):
        ruta = self._descargar_recurso(audio['url'])
        return {"name": audio['name'], "local_path": ruta} if ruta else None

# ===================
# CLASE YOUTUBE MANAGER (COMPLETA)
# ===================
class YouTubeManager:
    def __init__(self):
        self.youtube = self._autenticar()
        self._verificar_conexion()

    def _autenticar(self):
        try:
            logging.info("Iniciando autenticación YouTube...")
            creds = Credentials(
                token=None,
                refresh_token=CONFIG['YOUTUBE_CREDS']['refresh_token'],
                client_id=CONFIG['YOUTUBE_CREDS']['client_id'],
                client_secret=CONFIG['YOUTUBE_CREDS']['client_secret'],
                token_uri="https://oauth2.googleapis.com/token",
                scopes=['https://www.googleapis.com/auth/youtube']
            )
            creds.refresh(Request())
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            logging.critical(f"Error autenticación: {str(e)}")
            return None

    def _verificar_conexion(self):
        if not self.youtube:
            logging.critical("No se pudo establecer conexión con YouTube API")
            sys.exit(1)

    def crear_transmision(self, titulo, imagen_path):
        try:
            programacion = datetime.utcnow() + timedelta(minutes=5)
            logging.info(f"Programando transmisión para {programacion.isoformat()}Z")
            
            # Crear broadcast
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "24/7 Música Relajante • Relax Station Radio\n\nTransmisión continua de música ambiental",
                        "scheduledStartTime": programacion.isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True
                    }
                }
            ).execute()
            logging.info(f"Broadcast creado: {broadcast['id']}")
            
            # Crear stream
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
            logging.info(f"Stream creado: {stream['id']}")
            
            # Vincular broadcast y stream
            self.youtube.liveBroadcasts().bind(
                part="id,contentDetails",
                id=broadcast['id'],
                streamId=stream['id']
            ).execute()
            logging.info("Recursos vinculados correctamente")
            
            # Subir miniatura
            if imagen_path and os.path.exists(imagen_path):
                self._subir_miniatura(broadcast['id'], imagen_path)
            
            return {
                "rtmp_url": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id'],
                "scheduled_start": programacion
            }
        except Exception as e:
            logging.error(f"Error creación transmisión: {str(e)}")
            return None

    def _subir_miniatura(self, video_id, ruta_imagen):
        try:
            logging.info(f"Subiendo miniatura: {ruta_imagen}")
            self.youtube.thumbnails().set(
                videoId=video_id,
                media_body=ruta_imagen
            ).execute()
            logging.info("Miniatura actualizada correctamente")
        except Exception as e:
            logging.error(f"Error subiendo miniatura: {str(e)}")

    def obtener_estado_stream(self, stream_id):
        try:
            respuesta = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            return respuesta.get('items', [{}])[0].get('status', {}).get('streamStatus')
        except Exception as e:
            logging.error(f"Error obteniendo estado: {str(e)}")
            return None

    def transicionar_estado(self, broadcast_id, estado):
        try:
            logging.info(f"Transicionando a {estado.upper()}...")
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado.lower(),
                id=broadcast_id,
                part="status"
            ).execute()
            logging.info(f"Transición a {estado.upper()} exitosa")
            return True
        except Exception as e:
            logging.error(f"Error en transición: {str(e)}")
            return False

    def finalizar_transmision(self, broadcast_id):
        try:
            logging.info("Iniciando cierre de transmisión...")
            self.youtube.liveBroadcasts().transition(
                broadcastStatus="complete",
                id=broadcast_id,
                part="status"
            ).execute()
            logging.info("Transmisión finalizada correctamente")
            return True
        except Exception as e:
            logging.error(f"Error finalizando: {str(e)}")
            return False

# ===================
# FUNCIONES AUXILIARES (COMPLETAS)
# ===================
def generar_titulo(imagen):
    temas = ["Música para Relajar", "Sonidos Ambientales", "Mix Continuo", "Música para Trabajar"]
    return f"{random.choice(temas)} • {imagen['name']} • {datetime.utcnow().strftime('%H:%M UTC')}"

def generar_playlist(canciones, directorio):
    try:
        playlist_path = os.path.join(directorio, "playlist.m3u")
        with open(playlist_path, 'w') as f:
            f.write("#EXTM3U\n")
            for cancion in canciones:
                f.write(f"#EXTINF:-1,{cancion['name']}\nfile://{cancion['local_path']}\n")
        return playlist_path
    except Exception as e:
        logging.error(f"Error generando playlist: {str(e)}")
        return None

def manejar_transmision(stream_data, youtube, imagen, playlist_path):
    proceso_ffmpeg = None
    try:
        logging.info("\n" + "="*50)
        logging.info("INICIANDO TRANSMISIÓN")
        logging.info(f"Imagen: {imagen['name']}")
        logging.info(f"RTMP: {stream_data['rtmp_url']}")
        logging.info(f"Programado: {stream_data['scheduled_start']}")
        logging.info("="*50 + "\n")
        
        # Configurar FFmpeg
        cmd = [
            "ffmpeg",
            "-loglevel", "debug",
            "-re",
            "-stream_loop", "-1",
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
            "-bufsize", CONFIG['FFMPEG_PARAMS']['bufsize'],
            "-r", CONFIG['FFMPEG_PARAMS']['fps'],
            "-g", CONFIG['FFMPEG_PARAMS']['gop'],
            "-c:a", CONFIG['FFMPEG_PARAMS']['audio_codec'],
            "-b:a", CONFIG['FFMPEG_PARAMS']['audio_bitrate'],
            "-f", "flv",
            stream_data['rtmp_url']
        ] 
        
        # Iniciar FFmpeg
        proceso_ffmpeg = subprocess.Popen(
            cmd, 
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Loggear salida de FFmpeg
        def log_ffmpeg():
            while True:
                linea = proceso_ffmpeg.stderr.readline()
                if not linea and proceso_ffmpeg.poll() is not None:
                    break
                if linea:
                    logging.info(f"FFMPEG: {linea.strip()}")
        
        hilo_log = threading.Thread(target=log_ffmpeg, daemon=True)
        hilo_log.start()
        
        # Esperar activación del stream
        tiempo_inicio = time.time()
        while (time.time() - tiempo_inicio) < CONFIG['STREAM_STATES']['activation_timeout']:
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            logging.info(f"Estado del stream: {estado}")
            
            if estado == "active":
                if youtube.transicionar_estado(stream_data['broadcast_id'], "testing"):
                    logging.info("TRANSICIÓN A TESTING EXITOSA")
                    break
            time.sleep(CONFIG['STREAM_STATES']['check_interval'])
        else:
            raise Exception("Timeout: El stream no se activó")
        
        # Esperar hora programada
        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"Esperando {tiempo_restante:.1f}s para LIVE...")
            time.sleep(tiempo_restante)
        
        # Transicionar a LIVE
        if not youtube.transicionar_estado(stream_data['broadcast_id'], "live"):
            raise Exception("No se pudo iniciar transmisión LIVE")
        
        # Mantener transmisión
        tiempo_inicio_transmision = time.time()
        while (time.time() - tiempo_inicio_transmision) < CONFIG['STREAM_DURATION']:
            if proceso_ffmpeg.poll() is not None:
                logging.warning("Reiniciando FFmpeg...")
                proceso_ffmpeg = subprocess.Popen(cmd, stderr=subprocess.PIPE)
                hilo_log = threading.Thread(target=log_ffmpeg, daemon=True)
                hilo_log.start()
            time.sleep(15)
        
        # Finalizar
        proceso_ffmpeg.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        return True
        
    except Exception as e:
        logging.error(f"ERROR EN TRANSMISIÓN: {str(e)}")
        if proceso_ffmpeg:
            proceso_ffmpeg.kill()
        try:
            youtube.finalizar_transmision(stream_data['broadcast_id'])
        except Exception as final_error:
            logging.error(f"Error finalizando: {str(final_error)}")
        return False

# ===================
# CICLO PRINCIPAL (COMPLETO)
# ===================
def ciclo_principal():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            # Seleccionar contenido
            imagen = random.choice(gestor.medios['imagenes'])
            canciones = random.sample(
                [m for m in gestor.medios['musica'] if m['local_path']],
                min(50, len(gestor.medios['musica']))
            )
            
            # Generar playlist
            playlist_path = generar_playlist(canciones, CONFIG['CACHE_DIR'])
            if not playlist_path:
                time.sleep(CONFIG['RETRY_DELAY'])
                continue
                
            # Crear transmisión
            stream_data = youtube.crear_transmision(
                titulo=generar_titulo(imagen),
                imagen_path=imagen['local_path']
            )
            if not stream_data:
                time.sleep(CONFIG['RETRY_DELAY'])
                continue
            
            # Manejar transmisión en hilo separado
            threading.Thread(
                target=manejar_transmision,
                args=(stream_data, youtube, imagen, playlist_path),
                daemon=True
            ).start()
            
            # Esperar hasta próxima transmisión
            time.sleep(CONFIG['STREAM_DURATION'] + 300)
            
        except Exception as e:
            logging.error(f"ERROR EN CICLO: {str(e)}")
            time.sleep(CONFIG['RETRY_DELAY'])

@app.route('/health')
def health_check():
    return "OK", 200

def manejar_apagado(sig, frame):
    logging.info("\n🔴 Apagado solicitado...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, manejar_apagado)
    signal.signal(signal.SIGTERM, manejar_apagado)
    
    logging.info("\n" + "="*50)
    logging.info("INICIANDO RADIO 24/7")
    logging.info("="*50 + "\n")
    
    threading.Thread(target=ciclo_principal, daemon=True).start()
    
    serve(app, host='0.0.0.0', port=10000)
