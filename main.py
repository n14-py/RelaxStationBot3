import os
import random
import subprocess
import logging
import time
import requests
import hashlib
import socket
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
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.jpg")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Procesando imagen: {url}")
            temp_path = os.path.join(self.media_cache_dir, f"temp_{nombre_hash}")
            
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            subprocess.run([
                "ffmpeg", "-y", "-i", temp_path,
                "-vf", "scale=1280:720,setsar=1",
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
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
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
            
            logging.info("✅ Medios cargados exitosamente")
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
            return build('youtube', 'v3', credentials=creds, cache_discovery=False)
        except Exception as e:
            logging.error(f"🚨 Error de autenticación: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=3)
            
            broadcast_body = {
                "snippet": {
                    "title": titulo,
                    "description": "🎵 Música Continua 24/7 • Transmisión Automática\n🔔 Activa las notificaciones",
                    "scheduledStartTime": scheduled_start.isoformat() + "Z"
                },
                "status": {
                    "privacyStatus": "public",
                    "selfDeclaredMadeForKids": False,
                    "enableAutoStart": True,
                    "enableAutoStop": True,
                    "enableArchive": True,
                    "latencyPreference": "ultraLow"
                },
                "contentDetails": {
                    "enableLowLatency": True
                }
            }
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status,contentDetails",
                body=broadcast_body
            ).execute()
            
            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {"title": "Radio Automática 24/7"},
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
            
            logging.info(f"📡 Transmisión creada: {broadcast['id']}")
            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "scheduled_start": scheduled_start,
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id']
            }
        except Exception as e:
            logging.error(f"🚨 Error creando transmisión: {str(e)}")
            return None
    
    def obtener_estado_stream(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            return response['items'][0]['status']['streamStatus'] if response.get('items') else None
        except Exception as e:
            logging.error(f"Error obteniendo estado: {str(e)}")
            return None
    
    def obtener_estado_broadcast(self, broadcast_id):
        try:
            response = self.youtube.liveBroadcasts().list(
                part="status",
                id=broadcast_id
            ).execute()
            return response['items'][0]['status']['lifeCycleStatus'] if response.get('items') else None
        except Exception as e:
            logging.error(f"Error obteniendo estado broadcast: {str(e)}")
            return None
    
    def transicionar_estado(self, broadcast_id, estado):
        try:
            estado_actual = self.obtener_estado_broadcast(broadcast_id)
            if estado_actual == estado:
                logging.info(f"Estado actual ya es {estado}, omitiendo transición")
                return True
            
            logging.info(f"🔄 Transicionando de {estado_actual} a {estado}")
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="id,status"
            ).execute()
            logging.info(f"✅ Transición a {estado} exitosa")
            return True
        except Exception as e:
            logging.error(f"🚨 Error transicionando a {estado}: {str(e)}")
            return False
    
    def limpiar_transmisiones_fallidas(self, broadcast_id):
        try:
            self.youtube.liveBroadcasts().delete(id=broadcast_id).execute()
            logging.info(f"🧹 Transmisión fallida {broadcast_id} eliminada")
        except Exception as e:
            logging.error(f"Error limpiando transmisión: {str(e)}")

def verificar_conexion_rtmp(rtmp_url):
    try:
        parsed = urlparse(rtmp_url)
        host = parsed.hostname
        port = parsed.port or 1935
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(10)
            s.connect((host, port))
            logging.info("✅ Conexión RTMP verificada")
            return True
    except Exception as e:
        logging.error(f"🚨 Fallo conexión RTMP: {str(e)}")
        return False

def generar_titulo(imagen):
    temas = {
        'lofi': ['Lofi Vibes', 'Chill Beats', 'Study Mix'],
        'jazz': ['Jazz Night', 'Smooth Jazz', 'Blues Lounge'],
        'clasica': ['Clásica Relax', 'Piano Clásico', 'Sinfonías']
    }
    nombre = imagen['name'].lower()
    tema = next((k for k, v in temas.items() if any(x in nombre for x in v)), 'lofi')
    return f"{random.choice(temas[tema])} • {datetime.utcnow().strftime('%H:%M UTC')}"

def manejar_transmision(stream_data, youtube):
    try:
        logging.info(f"⏳ Iniciando transmisión programada para {stream_data['scheduled_start']}")
        
        if not verificar_conexion_rtmp(stream_data['rtmp']):
            raise Exception("Fallo en conexión RTMP")
        
        fifo_path = os.path.join(stream_data['gestor'].media_cache_dir, "audio_fifo")
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)
        
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
            "-b:v", "2500k",
            "-maxrate", "2500k",
            "-bufsize", "5000k",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "128k",
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        proceso = subprocess.Popen(ffmpeg_cmd)
        logging.info("🟢 FFmpeg iniciado - Transmitiendo contenido")
        
        # Esperar activación del stream
        activo = False
        for _ in range(30):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == 'active':
                logging.info("✅ Stream activo detectado")
                activo = True
                break
            logging.info(f"⌛ Estado actual del stream: {estado}")
            time.sleep(10)
        
        if not activo:
            raise Exception("El stream no se activó en 5 minutos")
        
        # Transicionar a testing
        if not youtube.transicionar_estado(stream_data['broadcast_id'], 'testing'):
            raise Exception("Fallo en transición a testing")
        
        # Esperar inicio programado
        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"⏳ Esperando {tiempo_restante:.1f}s para LIVE...")
            time.sleep(tiempo_restante)
        
        # Transicionar a live
        if not youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            raise Exception("Fallo en transición a live")
        
        logging.info("🎥 Transmisión LIVE en progreso")
        
        # Mantener transmisión por 8 horas
        start_time = datetime.utcnow()
        while (datetime.utcnow() - start_time) < timedelta(hours=8):
            musica = random.choice([m for m in stream_data['gestor'].medios['musica'] if m['local_path']])
            logging.info(f"🎵 Reproduciendo: {musica['name']}")
            
            with open(musica['local_path'], 'rb') as f:
                with open(fifo_path, 'wb') as fifo:
                    fifo.write(f.read())
            
            time.sleep(1)
        
        proceso.terminate()
        logging.info("🛑 Finalizando transmisión normalmente")
        youtube.transicionar_estado(stream_data['broadcast_id'], 'complete')

    except Exception as e:
        logging.error(f"🚨 Error crítico en transmisión: {str(e)}")
        youtube.limpiar_transmisiones_fallidas(stream_data['broadcast_id'])
    finally:
        if 'proceso' in locals() and proceso.poll() is None:
            proceso.kill()

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                logging.warning("⚠️ No hay medios disponibles, reintentando...")
                time.sleep(60)
                continue
            
            imagen = random.choice([i for i in gestor.medios['imagenes'] if i['local_path']])
            logging.info(f"🖼️ Imagen seleccionada: {imagen['name']}")
            
            titulo = generar_titulo(imagen)
            stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
            
            if not stream_info:
                time.sleep(60)
                continue
            
            stream_data = {
                "rtmp": stream_info['rtmp'],
                "scheduled_start": stream_info['scheduled_start'],
                "broadcast_id": stream_info['broadcast_id'],
                "stream_id": stream_info['stream_id'],
                "imagen": imagen,
                "gestor": gestor
            }
            
            logging.info(f"⏳ Próxima transmisión a las {stream_info['scheduled_start'].strftime('%H:%M UTC')}")
            
            hilo = threading.Thread(target=manejar_transmision, args=(stream_data, youtube))
            hilo.start()
            
            tiempo_espera = (stream_info['scheduled_start'] - datetime.utcnow()).total_seconds() + 28800 + 300
            if tiempo_espera > 0:
                logging.info(f"⏳ Próximo ciclo en {tiempo_espera//3600:.0f}h {((tiempo_espera%3600)//60):.0f}m")
                time.sleep(tiempo_espera)
            
        except Exception as e:
            logging.error(f"🚨 Error en ciclo principal: {str(e)}")
            time.sleep(30)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando Radio Automática 24/7...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
